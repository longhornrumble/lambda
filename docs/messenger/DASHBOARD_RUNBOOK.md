# Meta App Dashboard — webhook field subscriptions (M1a runbook step)

> Seed doc. M-Ha consolidates operator procedures into the Messenger Ops Runbook
> (picasso repo `docs/runbooks/MESSENGER_OPS.md`); until then this is the
> authoritative list of dashboard-side subscriptions the pipeline expects.

Payload v2 (contract C1) classifies edit/delete/standby events — but Meta only
delivers them if the app is **subscribed to the fields** in the App Dashboard
(App → Webhooks → Edit subscriptions), and per-Page via the subscribed_apps
edge. Operator steps:

## Facebook (object `page`)

Required (already subscribed as-built): `messages`, `messaging_postbacks`.

Add for M1a+:

Subscribe NOW (M1a):

| Field | Delivers | Consumed by |
|---|---|---|
| `message_edits` | `messaging[].message_edit` | M1b (history hygiene) |
| `message_deletions` | `messaging[].delete.mids[]` | M1b (Meta terms: delete stored copies) |

Subscribe at M6b — NOT before (each echo/standby event costs a webhook round-trip
+ mapping read + dedup write + processor invoke that nothing consumes until M6b):

| Field | Delivers | Consumed by |
|---|---|---|
| `message_echoes` | `messaging[].message.is_echo` | M6b (staff-reply pause) |
| `messaging_standby` | `entry.standby[]` | M6b (coexistence) |

Optional/deferred: `messaging_referrals` (logged skip today; future ref-param lane).

## Instagram (object `instagram`)

Required (already subscribed as-built): `messages`.

Subscribe NOW (M1a): `message_edit` (IG edits — M1b).

Subscribe at M6b — NOT before (same cost rationale as FB): `message_echoes`,
`standby`.

Notes:
- IG deletions arrive as `message.is_deleted` on the `messages` field — no
  separate subscription.
- After changing app-level subscriptions, re-check the **per-Page** subscribed
  fields (`POST /{page-id}/subscribed_apps`) — app-level and page-level lists
  are separate.
- Verification: send a test DM, edit it, delete it; the webhook CloudWatch log
  shows `Queued edit event` / `Queued delete event` (or `Intentional skip` for
  metadata-only shapes). Zero silent drops is the M1a DONE line.
