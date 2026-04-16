# Meta App Review Checklist — Phase 4A
**App:** MyRecruiter.ai  
**Purpose:** Platform-level Advanced Access for `pages_messaging`  
**Last updated:** 2026-04-15  
**Review timeline:** 2–8 weeks after submission

---

## Overview

This is a one-time submission to unlock Advanced Access permissions for the Picasso Messenger integration. Submit as early as possible — development mode testing continues uninterrupted while review is pending (up to 25 test users).

---

## 1. Permissions Summary

| Permission | Access Level | Status |
|---|---|---|
| `pages_show_list` | Standard | No review required |
| `pages_messaging` | **Advanced** | Core — submit for review |
| `pages_read_engagement` | Advanced | Submit for review (future comment→DM flows) |
| `instagram_basic` | Standard | No review required |
| `instagram_manage_messages` | Advanced | Submit for review |

Submit all Advanced permissions in a single review request. Batching avoids a second review cycle when Instagram DMs go live.

---

## 2. Submission Materials Checklist

### Required Before Submitting

- [ ] Working bot in development mode — test user sends a message, receives an AI response
- [ ] Privacy policy live at a public URL and explicitly mentioning messaging data
- [ ] Terms of service live at a public URL
- [ ] App icon uploaded (1024x1024 PNG, no alpha channel)
- [ ] Business verification complete or actively in progress (Meta Dashboard > Settings > Business Verification)
- [ ] At least 2 Meta accounts added as Test Users (App Dashboard > Roles > Test Users)

### Screencast (2–3 minutes)

Record a single continuous walkthrough. Do not cut between scenes — Meta reviewers look for seamless flows.

- [ ] Nonprofit admin connects their Facebook Page via the Config Builder OAuth flow
- [ ] Test user sends an initial message to the connected Page
- [ ] AI response arrives in Messenger within 5 seconds
- [ ] Multi-turn conversation — at least 3 back-and-forth exchanges
- [ ] Get Started postback button triggers correctly
- [ ] At least one quick reply button shown and tapped
- [ ] Screen clearly shows the Page name, the Messenger thread, and timestamps

### Per-Permission Justification (one paragraph each — see Section 3)

- [ ] `pages_messaging` justification written and attached
- [ ] `pages_read_engagement` justification written and attached
- [ ] `instagram_manage_messages` justification written and attached

### App Dashboard Configuration

- [ ] Messenger webhook callback URL set to the `Meta_Webhook_Handler` Lambda URL
- [ ] Webhook verify token matches `WEBHOOK_VERIFY_TOKEN` environment variable
- [ ] Webhook subscribed to: `messages`, `messaging_postbacks`, `messaging_optins`
- [ ] Valid OAuth redirect URIs include the Config Builder production URL

---

## 3. Per-Permission Justification Templates

Use these as a starting point. Personalize with your actual privacy policy URL and any nonprofit-specific details before submitting.

### `pages_messaging` (Advanced Access)

MyRecruiter.ai provides AI-powered chat for nonprofit organizations. When a nonprofit connects their Facebook Page to MyRecruiter.ai, visitors who message that Page receive instant, accurate responses about volunteer opportunities, donation programs, and eligibility requirements — sourced from the nonprofit's own knowledge base. Without `pages_messaging`, the platform cannot receive inbound messages from constituents or send the AI-generated replies that are the core value of the product. This permission is requested at the platform level because MyRecruiter.ai manages messaging on behalf of multiple nonprofit Pages, each of which has authorized the connection via OAuth.

### `pages_read_engagement` (Advanced Access)

MyRecruiter.ai plans to support comment-to-DM workflows, where a constituent who comments on a nonprofit's Facebook post is automatically offered a private Messenger conversation for more detailed assistance. Reading post engagement is required to identify relevant comments and trigger that handoff. This reduces friction for constituents who prefer to ask sensitive questions (such as eligibility for housing assistance) privately rather than in a public comment thread.

### `instagram_manage_messages` (Advanced Access)

Many nonprofits in the MyRecruiter.ai platform operate Instagram Professional accounts alongside their Facebook Pages. Constituents frequently reach out via Instagram DMs to ask the same questions about programs and volunteer opportunities. This permission allows MyRecruiter.ai to extend the same AI-powered response capability to Instagram DMs, giving nonprofits a unified inbox experience without requiring their staff to monitor and manually respond across two channels.

---

## 4. Pre-Submission Verification Checklist

Run through these immediately before hitting Submit in the App Dashboard.

**Webhook**
- [ ] Send a GET request to the Lambda URL with `hub.mode=subscribe`, `hub.verify_token`, and `hub.challenge` — confirm it echoes back the challenge value
- [ ] Send a test message from a test user account — confirm the bot responds in under 5 seconds

**App Dashboard**
- [ ] Messenger product added and configured
- [ ] Callback URL and verify token saved and validated (green checkmark in Dashboard)
- [ ] Webhook fields subscribed: `messages`, `messaging_postbacks`, `messaging_optins`
- [ ] OAuth redirect URIs saved

**Legal and Identity**
- [ ] Privacy policy URL loads without redirect errors
- [ ] Privacy policy text references Facebook/Instagram messaging and data retention
- [ ] Terms of service URL loads without redirect errors
- [ ] App icon visible in the Dashboard preview (correct dimensions, no broken image)
- [ ] Business verification status is Verified or Pending (Unverified will block Advanced Access)

**Test Users**
- [ ] At least 2 Meta accounts listed under App Dashboard > Roles > Test Users
- [ ] Test users can send messages to the connected test Page and receive responses

---

## 5. Post-Submission Notes

- **Timeline:** 2–8 weeks. No action required unless Meta contacts you.
- **Development continues:** Dev mode supports up to 25 test users while review is pending. All testing, QA, and nonprofit onboarding pilots can proceed.
- **Meta may request:** additional information via the developer support portal, or a live screen share demo. Respond within 5 business days to avoid the request expiring.
- **If rejected:** the rejection notice specifies which permission failed and why. Address the cited issues and resubmit — there is no mandatory cooldown period between submissions.
- **Monitoring:** check the App Review status in Meta App Dashboard > App Review every 1–2 weeks. Status moves from Pending Review → In Review → Approved/Rejected.
- **After approval:** Advanced Access is granted at the platform level. All connected Pages inherit it automatically — no per-Page re-authorization is needed.
