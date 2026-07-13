# Messenger Channel — Frozen Contracts (M0)

**Program:** Messenger Channel Experience — plan at picasso repo `docs/roadmap/MESSENGER_CHANNEL_EXPERIENCE.md` (§5 is the contract index; this file is the authoritative text).
**Frozen:** 2026-07-13 (M0), against lambda repo `origin/main@43bbdea`. Each contract carries its own version stamp.
**Vocabulary:** "Messenger" = Facebook Messenger + Instagram DM together. "FB" / "IG" where they differ.

**Amendment rule:** these contracts are FROZEN. A change is an M0-amendment PR to this file, reviewed by tech-lead-reviewer, with every named consumer subphase checked for impact. C1 amendments must be additive-only and observe the webhook-deploys-first rule (C1 §Deploy ordering). In-place edits without an amendment PR are prohibited.

**Schema discipline (applies to every contract):** every reader tolerates missing fields (CLAUDE.md Schema Discipline). New fields are additive; old-shape records must never crash a reader. PRs that add fields to a stored shape add an old-shape fixture test in the same PR.

---

## C1 — Invoke payload v2 · v1.0 (2026-07-13)

**Consumed by:** M1a (implements), M1b, M3b, M4, M6b, M-Ha.

The payload `Meta_Webhook_Handler` sends via async `InvokeCommand` to `Meta_Response_Processor` (`Meta_Webhook_Handler/index.js:407-416` today).

### v1 fields (frozen forever — names, types, and semantics byte-preserved)

```js
{
  psid:        string,        // sender PSID (FB) / IGSID (IG)
  messageText: string|null,   // message.text, or postback.payload for postbacks
  pageId:      string,        // entry.id
  tenantId:    string,
  tenantHash:  string,
  channelType: 'messenger' | 'instagram',
  messageMid:  string|null,   // message.mid, or synthetic `postback_{psid}_{ts}` for postbacks
  isPostback:  boolean,
}
```

### v2 additions (all additive; absent ⇒ reader treats as v1)

```js
{
  v: 2,                        // payload schema version marker
  eventKind: 'text' | 'postback' | 'quick_reply' | 'attachment' | 'sticker'
           | 'edit' | 'delete' | 'echo' | 'unsupported',
           // standby-ness is NOT an eventKind — it rides the isStandby flag (v1.1)
  timestamp: number,           // Meta messaging[].timestamp (epoch ms) — REQUIRED in every v2 payload
  quickReplyPayload: string|null,  // message.quick_reply.payload (C3 namespace); null unless eventKind='quick_reply'
  appId: string|null,          // message.app_id where Meta provides it (echoes); else null
  attachmentTypes: string[],   // attachment `type` values only (image|video|audio|file|sticker|…) — NO urls/content (PII minimization)
  targetMid: string|null,      // the mid being edited/deleted; only for eventKind edit|delete
  editedText: string|null,     // new text for eventKind='edit'; else null
  isStandby: boolean,          // true when delivered on the standby channel (Conversation Routing)
  replyTo: { mid?: string, storyUrl?: string } | null,  // IG story replies / reply-to context (research 03); else null
}
```

**`psid` inversion on echoes:** Meta inverts sender/recipient on echo events — the business's own account is `sender.id`, the customer is `recipient.id` (research report 04, IG echo marker). For `eventKind:'echo'` (main or standby channel), **`psid` MUST be read from `recipient.id`**, so the session key `meta:{pageId}:{psid}` stays the customer's conversation. For every other kind, `psid` = `sender.id` as today.

**Echo `messageText` is ALWAYS null (v1.1, loop guard):** echoes carry `message.text` (our own bot reply), and a processor that reads it as a user turn would answer it — bot answering itself, an infinite loop. This is not hypothetical: the *legacy* processor treats any non-empty `messageText` as a user message, so forwarding echo text during the M1a→M1b deploy gap would loop live. The webhook therefore never populates `messageText` on echo payloads; M6b's pause logic needs only `psid`/`appId`/`timestamp`. (Also PII minimization — no need to re-ship reply text.)

**`timestamp` fallback (v1.1):** when Meta omits `messaging[].timestamp` on an event shape, the webhook stamps receipt time (`Date.now()`), so `timestamp` is present in every v2 payload as required. Receipt time is conservative for the 24h send-window guard (never older than the true event time).

### Classification rules (webhook side, M1a)

- `quick_reply` tap ⇒ `eventKind:'quick_reply'`, `messageText` = `message.text` (v1-compatible), `quickReplyPayload` = the payload. (QR taps arrive as `messages`, NOT postbacks — research report 01 §2.)
- Attachment-only message ⇒ `eventKind:'attachment'`, `messageText:null`, `attachmentTypes` populated. Text+attachment ⇒ `eventKind:'text'` with `attachmentTypes` populated.
- `message.is_echo` ⇒ `eventKind:'echo'`, `appId` populated, never invokes Bedrock (consumer: M6b pause logic).
- Standby-channel events ⇒ same classification with `isStandby:true`.
- Sticker: both pre- and post-Aug-30-2026 webhook shapes classify to `eventKind:'sticker'` (migration: sticker becomes an attachment `type` — M1a fixtures carry both shapes; the pre-migration shape already includes `sticker` in `attachmentTypes`, so one check covers both).
- IG story replies classify as their underlying kind (usually `text`) with `replyTo` populated.
- **Metadata-only events ⇒ logged intentional skip** (no invoke, no fallback reply — these carry no user input to answer): reactions, delivery/read receipts, standalone `messaging_referrals`, `response_feedback`. (Referral `ref`-param handling is a future additive amendment, not `unsupported`-fallback territory.)
- Anything else that DOES carry user input ⇒ `eventKind:'unsupported'` — invoked, never silently dropped (30-second rule, research report 02 §1).

### Deploy ordering (the rule that makes split PRs safe)

1. **Webhook deploys first.** Always. The processor is never updated to require a v2 field before the webhook ships it.
2. **Legacy-gap behavior is verified safe:** the current processor's `validateEvent` (`Meta_Response_Processor/index.js:656-663`) throws on empty `messageText` and the handler drops without retry — so v2 payloads for new event kinds (`messageText:null`) hitting the legacy processor produce a logged drop, identical to today's webhook-side drop. No crash, no regression. M1b then adds the real handling.
3. `timestamp` activates the processor's existing dormant 24h guard (`index.js:712`) immediately on M1a deploy — intended (stale-DLQ protection is the guard's purpose).
4. v1-shape contract fixtures live in BOTH functions' test suites; a change that breaks a v1 fixture fails CI.

---

## C2 — `messenger_behavior` config section + gating hierarchy · v1.0 (2026-07-13)

**Consumed by:** M1b, M3a, M5, M6a. Types land in CB repo `src/types/config.ts` (M0).

### The three gates (all must pass for Messenger behavior to run)

| Gate | Where | Meaning | Authority |
|---|---|---|---|
| 1. Connection | DDB `picasso-channel-mappings` item attr `enabled` (per page+channel) | this Page/IG account is connected & on | **Runtime-authoritative.** The webhook consults it (`Meta_Webhook_Handler/index.js:383`). Any `enabled` mirror inside S3 config (`channels.*.enabled`) is display-only for Config Builder; no sync is guaranteed; **no runtime code may ever read the S3 mirror for gating.** |
| 2. Tenant behavior flag | S3 config `feature_flags.MESSENGER_CHANNEL` | tenant is opted into the new Messenger experience (V5, CTAs, forms, …) | Flag name **finalized here: `MESSENGER_CHANNEL`**. Flag off ⇒ processor behavior byte-identical to the pre-program baseline (M1b/M3b contract tests). |
| 3. Tuning | S3 config `messenger_behavior` (top-level key) | how the experience behaves | All fields optional; missing section ⇒ built-in defaults. |

`messenger_behavior` is deliberately **NOT** under `channels` — `channels.*` is OAuth-connection state written by `Meta_OAuth_Handler`; two writers on one key is clobber risk (plan §4 D1).

### Section shape (v1 — all fields optional, additive evolution only)

```ts
interface MessengerBehaviorConfig {
  tone_override?: string;        // replaces config.tone_prompt in the Messenger prompt when set (C6)
  model_id?: string;             // Messenger model override (C6 precedence)
  max_history_turns?: number;    // default 5 (processor MAX_HISTORY_TURNS)
  strings?: MessengerStrings;    // ALL user-facing strings (D10 — Spanish i18n localizes by config)
  welcome?: MessengerWelcomeConfig;      // M5: ice breakers (≤4) + persistent menu source
  channel_overrides?: {          // per-channel deltas; same shape, applied last (C6)
    messenger?: MessengerChannelOverride;
    instagram?: MessengerChannelOverride;
  };
}

interface MessengerStrings {
  disclosure_line?: string;          // sent on first turn of a session (M1b; bot-disclosure rule)
  unsupported_input_fallback?: string; // reply to attachment/sticker/voice we can't process (M1b; 30s rule)
  escalation_confirmation?: string;  // "connecting you with a person…" (M6a)
  rate_limited?: string;             // polite throttle message (M-Hb)
  form_summary_intro?: string;       // generic form-flow strings (M7a); field prompts stay in conversational_forms
  [key: string]: string | undefined; // additive strings without a type change
}

interface MessengerChannelOverride {
  tone_override?: string;
  model_id?: string;
  strings?: MessengerStrings;
}

interface MessengerWelcomeConfig {
  ice_breakers?: Array<{ question: string; payload: string }>; // ≤4 (C5); payload in C3 namespace
  persistent_menu?: Array<{ title: string; payload?: string; url?: string }>; // title ≤20 chars wisdom applies per C5 refs
}
```

Defaults for every string live in code; config overrides them. Readers use `config.messenger_behavior?.field ?? default` — never bracket access.

---

## C3 — Structured payload namespace · v1.0 (2026-07-13)

**Consumed by:** M4, M5, M7a.

Format for every postback payload and quick-reply payload Picasso emits (Meta cap: 1000 chars — research report 01):

```
PIC1:<route>:<arg1>[:<arg2>…]
```

- `PIC1` = namespace + version. Future format changes bump to `PIC2`; parsers route on the prefix.
- Args are colon-delimited; args containing `:` are prohibited (route designers pick safe ids); no JSON (keeps payloads short and log-greppable).

### v1 routes

| Route | Shape | Emitted by | Handled by |
|---|---|---|---|
| `cta` | `PIC1:cta:{ctaId}` | CTA quick replies / ice breakers (M4, M5) | processor: execute the CTA (send_query text, etc.) |
| `ffld` | `PIC1:ffld:{formId}:{fieldKey}:{optionValue}` | form enum quick replies (M7a) | form engine: record answer |
| `fctl` | `PIC1:fctl:{formId}:{op}` where op ∈ confirm\|cancel | form summary/confirm QRs (M7a) | form engine |
| `sched` | `PIC1:sched:{op}:{arg}` (ops defined in M8a, e.g. `slot:{slotId}`) | scheduling QRs/carousel (M8a) | scheduling driver |

### Preserved behaviors (frozen)

- `GET_STARTED` (Meta's conventional get-started postback) is preserved as-is — NOT migrated into the namespace (`Meta_Response_Processor/index.js:782` handles it today).
- **Unknown-payload → RAG:** any payload not matching `PIC1:` (or `GET_STARTED`) is treated as free text into the normal RAG turn — today's behavior for all postbacks, and the forward-compat path for stale buttons after a format bump.

---

## C4 — `picasso-conversation-state` table schema · v1.0 (2026-07-13)

**Consumed by:** M1c (provisions), M6a, M7a, M8a, M-Hb.

- **Table:** `picasso-conversation-state` — bare name (account = environment; CLAUDE.md naming rule). On-demand billing. Provisioned by Terraform in the picasso repo (M1c) with per-function IAM (never shared roles).
- **PK** `sessionId` (S) — for Messenger: `meta:{pageId}:{psid}` (matches the history-table namespace). Other channels may adopt the table later with their own id shapes.
- **SK** `stateType` (S).
- **TTL attribute: `expires_at`** (N, epoch **seconds**) — named here to kill the `ttl`-vs-`expires_at` bug class (plan §3 fact 6). Every row MUST set it.

### Common attributes (every row)

```
sessionId (S), stateType (S), expires_at (N, epoch s), updated_at (N, epoch ms), schema_version (N) = 1
```

### Row shapes (readers tolerate missing fields; writers may add fields additively)

| stateType | Attributes (beyond common) | Written by | TTL guidance |
|---|---|---|---|
| `lock` | `owner` (S, Lambda request id), `acquired_at` (N ms), `pending` (L of M: `{timestamp (N), mid (S), text (S, absent for non-text kinds), eventKind (S, optional), quickReplyPayload (S, optional), attachmentTypes (L, optional)}`) | M1c serialization (C7) | lock TTL invariant in C7 |
| `pause` | `reason` (S: `escalation`\|`echo_watch`), `paused_at` (N ms) | M6a/M6b | 24 h (thread-control idle expiry) |
| `form_session` | `form_id` (S), `current_field` (S), `answers` (M), `started_at` (N ms) | M7a | idle TTL decided in M7a (G-P3); contract fixes only the attr names |
| `scheduling_session` | `program_id` (S), `stage` (S), `selected_slot` (M), `contact` (M) | M8a | decided in M8a (G-P4) |
| `counters` | `window_start` (N ms), `turn_count` (N) | M-Hb throttles | window-sized |

New stateTypes are additive (no amendment needed); changing an existing row shape IS an amendment.

**PII note:** `answers` / `contact` hold user PII → the table is a PII surface; M1c updates `pii-inventory.md` + data-classification in the same PR (Living-Inventory rule), and TTLs are the retention mechanism.

---

## C5 — Capability map · v1.0 (2026-07-13)

**Consumed by:** M1b, M3a, M4. Source: research pack (`Facebook/messenger-research-2026-07/`, reports 01–03), July 2026.

| Capability | FB Messenger | Instagram DM |
|---|---|---|
| Message text cap | **2000 chars** | **1000 chars** |
| Quick replies | ≤ **13**, title ≤ **20 chars** (distinct from message caps), transient (vanish after next message) | same |
| `user_email` / `user_phone_number` quick replies | FB only | **not available** |
| Button template | ≤ **3** buttons, persistent in thread | same; structured templates **invisible on IG web** — verify in-app (M4) |
| Carousel (generic template) | ≤ **10** cards | same |
| Postback / QR payload size | ≤ 1000 chars | same |
| Typing indicator (`sender_action`) | supported (refresh ≤ ~10 s intervals) | **no-op** (processor `index.js:405` today) |
| Ice breakers | ≤ **4**, alive (Messenger Profile API) | ≤ 4, alive |
| Persistent menu | yes (menu tap **opens a fresh 24h window**) | yes |
| Inbound GIF/sticker webhook | fires | **fires NO webhook at all** |
| Message edit/delete webhooks | `message_edits` / `message_deletions` (subscribe — M1a runbook) | `message_edit`; `is_deleted` ⇒ Meta terms **require deleting stored copies** (M1b) |
| Post-window lanes | Utility templates / HUMAN_AGENT (App-Review-gated) / OTN | **NONE in the US** ⇒ IG bookings REQUIRE phone; SMS is the reminder rail (D9) |
| Sticker webhook migration | `sticker` attachment type from **Aug 30 2026** — M1a fixtures carry both shapes | same |

Rendering code reads caps from a single shared constant module (created in M1b), never inline literals.

---

## C6 — Prompt precedence · v1.0 (2026-07-13)

**Consumed by:** M3a.

Layers, lowest → highest; each layer overrides only the fields it sets:

1. **Code-owned Messenger base prompt** (M3a module): brevity/format rules (short-form, no markdown, ≤3 sentences), V5 splice points. **Not tenant-overridable** — locked rules stay locked.
2. `config.tone_prompt` — tenant persona (exactly as today, `buildMessengerPrompt` `index.js:548`).
3. `messenger_behavior.tone_override` — **replaces** `tone_prompt` in the Messenger prompt when set (replace, not concatenate).
4. `messenger_behavior.channel_overrides.{messenger|instagram}.tone_override` — replaces again, per channel.

V5 blocks (action catalog, turn check, tail instruction — `shared/prompt/` after M2) are spliced by code and are never part of the override surface.

**`model_id` precedence** (highest wins): `channel_overrides.{ch}.model_id` → `messenger_behavior.model_id` → `config.model_id` (today's only override, `index.js:588`) → processor `DEFAULT_MODEL_ID` (`index.js:75`).

---

## C7 — Per-conversation serialization · v1.0 (2026-07-13)

**Consumed by:** M1c (implements), M7a. Scope: per `sessionId` only — distinct sessions are never serialized against each other.

**Problem:** the webhook async-invokes one processor per inbound message; rapid-fire messages on one conversation run concurrently ⇒ interleaved replies, history read/write races, doubled Bedrock spend. The widget serializes naturally (one client, sequential requests); Messenger doesn't.

**Decision: single-writer lock + coalesced pending queue.** (Not drop, not wait.)

Why the alternatives lose (adversarial-pass evidence, not vibes):
- **Drop the loser** — violates the 30-second/no-silent-drop rule (C1); user input vanishes.
- **Wait/retry the loser** — burns a concurrent Lambda for the full turn (2–10 s Bedrock); N rapid messages ⇒ N−1 idle executions AND still N separate Bedrock calls racing history. Costs more and fixes nothing.
- **Lock + coalesce** — loser does one cheap conditional write and exits; winner answers all pending input in one combined turn. One Bedrock call per burst, coherent history, no lost input.

Mechanism (row shape in C4 `lock`):
1. **Acquire:** conditional `PutItem` of the `lock` row — succeeds iff `attribute_not_exists(sessionId)` OR `expires_at < now` (stale-lock takeover). Winner proceeds to the turn.
2. **Coalesce:** loser conditionally appends its pending item (C4 shape — carries the v2 fields, so non-text kinds coalesce too) to the winner's `pending` list and exits (never calls Bedrock). If the append races a lock delete (winner just finished), the loser retries acquisition once — then proceeds as a winner.
3. **Drain:** after sending its reply, the winner re-reads the lock row; if `pending` non-empty, it consumes ALL pending items in timestamp order as one drain cycle: structured members (`quick_reply` with a `PIC1:` payload) route via their C3 handlers individually; free-text members are newline-joined into ONE combined user turn (one Bedrock call); non-text unsupported members contribute a single deduped `unsupported_input_fallback` reply.
4. **Release is CONDITIONAL — never drop-on-release:** delete the lock row gated on `pending` being empty (`attribute_not_exists(pending) OR size(pending) = 0`). If the condition fails (a loser appended between drain-check and delete), that is another drain cycle — go to step 3.
5. **Drain cap bounds spend, not delivery:** at most **3 Bedrock-calling drain cycles** per lock hold. Cycles beyond the cap still consume `pending` (no-drop) but reply with the C2 `rate_limited` string instead of calling Bedrock — bounded cost, every message still answered. Sustained flood beyond that is M-Hb throttle territory.
6. **Lock TTL invariant:** lock TTL **≥ processor function timeout + 10 s margin** (function timeout is 120 s today — picasso repo `infra/modules/lambda-meta-staging/main.tf`; so lock TTL ≥ 130 s). The TTL and the function timeout are coupled: **raising the function timeout requires amending this contract** and the lock-module constant together, or a legitimately-running turn can have its lock stolen mid-processing. `expires_at`-based takeover (step 1) is the crash-recovery path.

Exact conditional expressions, retry jitter, and metrics are M1c implementation detail; frozen here: the mechanism (lock+coalesce), the row shape (C4), the no-drop guarantee (incl. conditional release), the drain semantics and cap, the TTL invariant, and per-sessionId scope.

---

## C8 — Session-boundary definition · v1.0 (2026-07-13)

**Consumed by:** M3a, M3b.

**Problem (G4):** Messenger threads are endless. V5's TURN-CHECK counts assistant questions over the whole history passed to it (`prompt_v5.js:127`) — unscoped, the funnel rules misfire forever after the bot's second-ever question.

**Definition:** a **session** is a maximal run of consecutive messages in the conversation history (`picasso-recent-messages`, `sessionId = meta:{pageId}:{psid}`) where the gap between consecutive `messageTimestamp` values (epoch ms, the table SK) is **< 86,400,000 ms (24 h)**. A gap **≥ 86,400,000 ms ⇒ new session** (boundary sits before the later message).

Rules:
- **History windowing:** the prompt receives only messages after the most recent boundary (capped by `max_history_turns`).
- **TURN-CHECK counting + sustained-interest evaluation:** current session only, never the lifetime thread.
- **Clock:** boundaries are computed from stored `messageTimestamp` write-times (one consistent clock — ours). The Meta event `timestamp` (C1) is for the 24h **send-window** guard, a separate concern; the two are never mixed.
- **Disclosure line** (C2 strings): sent on the first turn of each session, not only the first turn ever.
- **Edge cases:** exactly 86,400,000 ms ⇒ new session (≥ semantics); empty history ⇒ new session; the boundary check runs on history as-read (post-TTL-expiry rows simply absent — consistent with M1b's 7-day TTL, which can only shorten history, never bridge a gap).

---

## C9 — CTA rendering map + free-text fallback · v1.0 (2026-07-13)

**Consumed by:** M4 (implements — by name, no re-derivation), M7a, M8a.

| CTA shape | Renders as | Rationale |
|---|---|---|
| `send_query` / LEARN-intent suggestions | **Quick replies** (≤13, titles truncated to 20 chars per C5) | transient suggestion — matches QR transience |
| `external_link`, `start_form` (interim link-out until M7), commitment intents (APPLY / SCHEDULE) | **Button template `web_url` buttons** (≤3, persistent) | commitment CTAs must survive in the thread |
| >3 commitment CTAs in one turn | top 3 in V5-returned order; rest dropped (logged) | C5 hard cap |
| Form enum options (M7a) | quick replies with `PIC1:ffld:` payloads | |
| Scheduling slots (M8a) | carousel, ≤10 cards | C5 |

Attachment rule: renderings attach to the **same turn's send sequence** (no cross-turn state); QR set and buttons come from the turn's validated `actionIds` (`validateActionIds`, cap 4, `prompt_v5.js:191`). **Split-reply rule:** when the reply text is split across N Send API calls (C5 char caps), quick replies attach to the **final text chunk**; a button template is sent as its own message **after** the final text chunk (it is structurally a separate message). Nothing renders on intermediate chunks.

**Free-text-fallback principle (frozen):** every tap-driven flow MUST also accept the equivalent typed text — quick replies are skippable and vanish after the next message. A tap is an accelerator, never the only path. Every M4/M7/M8 flow ships a free-text-path test to CI.

---

## Version log

| Date | Change | PR |
|---|---|---|
| 2026-07-13 | C1–C9 v1.0 frozen (M0), after tech-lead-reviewer adversarial pass — 3 blocking + 4 should-fix findings applied pre-freeze (echo `psid` inversion; C7 conditional release + drain semantics + TTL invariant; C4 pending shape carries v2 fields; metadata-only-event skip rule; `replyTo` context; C9 split-reply rule) | lambda#433 |
| 2026-07-13 | **C1 → v1.1** (M1a implementation findings, additive clarifications): echo `messageText` always null (loop guard — legacy processor would answer our own echoed replies during the deploy gap); `timestamp` falls back to receipt time when Meta omits it; `'standby'` removed from the eventKind enum (standby-ness rides `isStandby`, matching the prose rule); NOTE for M1b: edit/delete bypass webhook dedup, so Meta redeliveries double-invoke — processor edit/delete handling MUST be idempotent | (M1a PR) |
