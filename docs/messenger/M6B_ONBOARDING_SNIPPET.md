# Tenant onboarding — bot ↔ staff coexistence (M6b)

> Drafted for M6b; destination is the picasso repo's Messenger Ops Runbook
> (`docs/runbooks/MESSENGER_OPS.md` §3 — operator moves this content there,
> the lambda repo owns no operator-facing runbook of its own). Covers the
> checklist a tenant needs so the bot and human staff can share one Page/IG
> inbox without both replying to the same message ("double-responder").

## Why this matters

Once a tenant connects a Page/IG account (`Meta_OAuth_Handler`) with
`feature_flags.MESSENGER_CHANNEL` on, the bot answers every inbound DM. If
staff ALSO reply from Meta Business Suite on the same thread — or another
connected tool sends a message — two systems can answer the same customer at
once unless one of them stands down. M6b gives the bot two independent ways
to detect "a human/other tool just replied" and stand down for ~24h:

1. **Thread-control handoff (M6a)** — when a user explicitly asks for a
   human, the bot calls `pass_thread_control` to hand the Page's default
   inbox app the conversation. Requires **Conversation Routing** to be
   configured (below).
2. **Echo-watch (M6b, belt-and-suspenders)** — works even if Conversation
   Routing was never set up: every reply on a thread (bot's own, staff's, or
   another tool's) generates a Meta "echo" webhook event carrying an
   `app_id`. If that `app_id` isn't ours, M6b treats it as a signal that a
   human/other tool is now active and writes the same C4 pause row directly
   — no dependency on thread-control state.

Both paths write to the same `picasso-conversation-state` `pause` row (C4);
either one standing the bot down is sufficient.

## Onboarding checklist

- [ ] **Our app is the Page's default application.** Meta Business Suite →
      Settings → this Page's connected apps → confirm the Picasso app is
      listed and set as default under **Conversation Routing** (formerly
      "Automated Assistant handover"). Conversation Routing is what makes
      `pass_thread_control` (M6a) actually move the thread instead of
      returning a non-2xx (which M6a treats as "not configured, proceed
      anyway" — the escalation confirmation, pause, and email still fire, but
      the thread does NOT visibly transfer in the Business Suite inbox UI
      without this step).
- [ ] **Take/pass thread control is enabled** for our app on this Page (same
      Conversation Routing screen — the toggle that allows an app to hand
      control back and forth). Without it, `pass_thread_control` calls fail
      even if our app is the default.
- [ ] **Business Suite instant-reply / away-message automations are OFF**
      for this Page. Meta's own auto-responders are a second bot answering
      the same inbound message the Picasso bot just answered — a
      double-responder hazard distinct from a staff member typing a reply.
      Business Suite → Automation → turn off "Instant Reply" and "Away
      Message" (or scope them to hours Picasso itself isn't handling, if the
      tenant insists on keeping one).
- [ ] **Meta Business AI Agent ("Meta Business Agent") is OFF** for this
      Page/IG account. This is Meta's own AI auto-responder product — running
      it alongside Picasso is the same double-responder hazard as the
      instant-reply automations above, except AI-generated (less
      predictable, harder for staff to notice conflicting replies).
- [ ] **Webhook fields subscribed** (see `docs/messenger/DASHBOARD_RUNBOOK.md`
      — subscribe at M6b, not before): `message_echoes` (FB) /
      `message_echoes` (IG) and `messaging_standby` (FB) / `standby` (IG).
      Without these subscriptions Meta never delivers the echo/standby
      events echo-watch and standby-consumption depend on — the bot would
      still work, but silently lose BOTH coexistence mechanisms (M6a's
      confirmation-and-pause still works via the user's own escalation
      request; echo-watch and standby-consumption specifically need these
      fields).
- [ ] **`messenger_behavior.escalation_email`** set in the tenant config if
      staff want a notification when a user asks for a human (optional — the
      handoff + pause proceed either way; C2).

## Verification steps (staging)

Do this on both a Facebook Page and an Instagram account connected to the
same test tenant:

1. **Send a test DM** to the connected Page/IG account from a personal
   test-user account. Confirm the bot replies normally.
2. **Reply as staff** from the Business Suite inbox (typing a real reply,
   not tapping "AI suggested reply" if that's a distinct Meta feature) on
   that same thread.
   - Expected: the next inbound message from the test user gets **no bot
     reply** — CloudWatch shows `Echo-watch pause written — foreign-app
     reply detected, bot standing down` (or, if Conversation Routing was
     already configured and the user had explicitly asked for a human, the
     M6a escalation path already paused it first — either is correct).
   - Verify the `picasso-conversation-state` table has a `pause` row for
     `meta:{pageId}:{psid}` with `reason` = `echo_watch` (or `escalation`)
     and `expires_at` ≈ now + 24h.
3. **Resume — wait it out (soak-friendly shortcut: don't actually wait 24h
   in staging)**: either
   - let the pause row's `expires_at` pass (24h) — the next inbound message
     gets a normal bot reply again, and the stale row is opportunistically
     deleted on that same read (best-effort; DynamoDB's own TTL sweep is the
     backstop if the opportunistic delete doesn't fire), OR
   - **explicit resume (NOT YET IMPLEMENTED)**: staff can hand the thread
     back via **Take Thread Control** in the Business Suite inbox, but our
     bot does NOT currently consume thread-control webhooks — the local
     pause row still gates replies until it expires (24h) or the
     opportunistic cleanup fires. Consuming pass/take_thread_control events
     to clear the pause early is a named follow-up. For staging
     verification, test the expiry path with a short-lived synthetic row
     rather than waiting a day.
4. **Double-responder regression check**: with Business Suite instant-reply
   OFF and Meta Business Agent OFF (checklist above), send a DM and confirm
   only ONE reply arrives (the bot's). If two replies arrive, one of those
   automations is still on for this Page.

## What "done" looks like

- A staff reply from the inbox visibly pauses the bot (CloudWatch log +
  `pause` row, verified live per step 2 above).
- The bot resumes after either the 24h idle expiry or an explicit
  Take Thread Control handoff (step 3).
- No double-reply is observed with the tenant's automations configured per
  the checklist (step 4).
