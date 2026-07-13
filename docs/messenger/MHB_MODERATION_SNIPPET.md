# Abuse & cost controls — rate limits + moderation (M-Hb)

> Drafted for M-Hb; destination is the picasso repo's Messenger Ops Runbook
> (`docs/runbooks/MESSENGER_OPS.md` §4 — operator moves this content there,
> the lambda repo owns no operator-facing runbook of its own). Covers the two
> config-driven throttles the processor enforces automatically, and the
> **operator procedure** for the one thing that isn't automated: blocking a
> sustained abuser via Meta's Moderate Conversations feature.

## Why this matters

Any DM to a connected Page/IG account reaches `Meta_Response_Processor`
without any login, CAPTCHA, or account creation — it is an unauthenticated
public path straight into a Bedrock call. That's an acceptable, expected
surface for one operator/one tenant during Meta's App Review "developer +
tester" allowlist window, but becomes a real spend risk once **Advanced
Access** ships and any member of the public can message a connected Page.
M-Hb adds two automatic, config-driven counters (`Meta_Response_Processor/rateLimits.js`)
so a flood — accidental (a broken client retry loop) or deliberate (a
scripted flood) — gets bounded, polite pushback instead of unbounded spend.

Moderation itself (blocking/reporting an abusive PSID) is **not** a built
tool — it is a manual step in the Business Suite inbox, documented below as
an operator procedure. The rate limiter buys time to do that; it does not
replace it.

## What the rate limits do

Two independent counters, both riding the C4 `picasso-conversation-state`
table (additive `stateType` rows — `rl_user:{yyyymmddHH}`, UTC hour bucket;
`rl_day:{yyyymmdd}`, UTC day bucket):

| Limit | Default | Config override | Scope |
|---|---|---|---|
| Per-PSID hourly turn count | **30** turns/hour | `messenger_behavior.rate_limits.per_user_hourly` | one Messenger/IG user, one tenant |
| Per-tenant daily turn count | **1000** turns/day | `messenger_behavior.rate_limits.tenant_daily` | every user of one tenant, combined |

```json
{
  "messenger_behavior": {
    "rate_limits": {
      "per_user_hourly": 30,
      "tenant_daily": 1000
    }
  }
}
```

Both fields are optional — omit either (or the whole `rate_limits` section)
to keep the code default. **v1 does not honor `channel_overrides`** for rate
limits — one pair of numbers applies to both Messenger and Instagram DM for
a tenant. (If a tenant later needs Messenger and Instagram tuned separately,
that's a small additive follow-up, not a redesign.)

Checked ONCE per winning invocation of the C7 per-conversation lock — after
the escalation check (see "escalation is never throttled" below), before any
Bedrock call. This also covers any C7 drain cycles that invocation goes on
to run: a coalesced burst is already combined into a single Bedrock call
(C7's own spend model), so one rate-limit check/increment per winning
invocation is the correct accounting unit — not one per message.

**Behavior once a limit is hit:**
- First 3 breaches past the limit → the bot still replies, with the
  polite, config-driven `rate_limited` string (`messenger_behavior.strings.rate_limited`;
  default: *"You're sending messages faster than I can keep up — one moment
  please."*).
- 4th+ consecutive breach in the same window → fully silent. No reply, no
  Bedrock call, no history write. This is deliberate — a sustained flood
  does not need four warnings, and every additional reply is itself more
  spend.
- Either way: no Bedrock call, no `picasso-recent-messages` history rows for
  a rate-limited turn.

**Escalation is never throttled.** The "talk to a human" detector
(`escalation.detectEscalationIntent`) runs *before* the rate-limit check. A
user who has been rate-limited can still reach a human by asking for one —
the limiter only ever suppresses bot replies, never a user's ability to
escalate.

**Fail-open.** If the `picasso-conversation-state` table is unreachable (or
any DynamoDB error occurs while bumping a counter), the limiter logs a WARN
and lets the turn proceed normally. A flaky counters table must never be the
reason a legitimate user gets no reply — this is a cost control, not an
availability gate.

## The `TENANT_DAILY_CAP` log marker

When the **tenant-wide** daily cap is hit, the processor logs a structured
WARN whose `message` field is the literal string `TENANT_DAILY_CAP`
(CloudWatch Logs Insights / a metric filter can watch for it directly). A
per-user hourly breach instead logs `RATE_LIMITED user`. Seeing
`TENANT_DAILY_CAP` repeatedly for one tenant is the signal that either:
- the tenant's real traffic has genuinely grown past 1000 turns/day (raise
  `messenger_behavior.rate_limits.tenant_daily` for that tenant), or
- one or more PSIDs are flooding hard enough, spread across enough hourly
  windows or distinct users, to add up to the tenant-wide cap — worth
  checking the per-PSID `RATE_LIMITED user` volume for the same window
  before assuming it's legitimate growth.

There is no CloudWatch alarm wired to this marker yet — for now it's a
grep/Insights-query target during an incident, not a paging alert.

## Operator procedure: blocking/reporting a PSID (Moderate Conversations)

The rate limiter buys time; it does not stop a determined abuser
indefinitely (three polite replies per window, forever, is still bounded
but non-zero spend). For sustained abuse from one person, use Meta's
**Moderate Conversations** feature directly from the Page/IG inbox. This is
a manual operator action — Picasso has no API integration with it and none
is planned.

**When to use this:** a specific PSID keeps hitting `RATE_LIMITED user` well
past the first-3-breaches polite window, session after session — i.e. the
automatic throttle is firing repeatedly for the same person, not a one-off
burst.

**How:**
1. Open **Meta Business Suite** → **Inbox** for the connected Page (or the
   Instagram professional account's inbox).
2. Find the conversation thread with the abusive PSID (search by name/last
   message, or cross-reference the `psid` from the `RATE_LIMITED user` log
   line against the inbox thread list — the log intentionally does not
   carry enough context to jump straight to the thread; matching by
   timestamp + recent message content in the inbox is the practical path).
3. Open the conversation, click the participant's name/profile chip →
   **Block** (or **Report**, if the content itself warrants it — spam,
   abuse, illegal content, etc., per Meta's own reporting flow).
4. Blocking prevents that PSID from messaging the Page/IG account at all —
   this stops the flood at the Meta platform level, upstream of Picasso
   entirely (no more webhook deliveries for that PSID, so the rate limiter
   never even sees future traffic from them).

**This is deliberately a one-operator, one-tenant manual procedure today**
— no bulk-block tooling, no cross-tenant blocklist, no API automation. If a
future phase needs bulk/cross-tenant moderation tooling, that's a new
scoped subphase, not a retrofit of this doc.

## What "done" looks like

- A flood test from one PSID gets the polite `rate_limited` reply for its
  first 3 breaches, then goes silent — verified against a live staging DM
  burst.
- A tenant-wide daily cap test produces the `TENANT_DAILY_CAP` log marker
  and a polite reply, without touching Bedrock.
- An operator can find and block a real abusive PSID via Business Suite
  inbox using the steps above.
