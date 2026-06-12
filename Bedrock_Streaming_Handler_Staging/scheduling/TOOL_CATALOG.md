# Agentic Scheduling — Tool Catalog (Phase-0, v1)

**Source of truth:** `FROZEN_CONTRACTS.md` §B17c (LOCKED 2026-06-12, incl. the governance +
PII advisory amendments). These two entries are the **full v1 catalog** — the platform's
first agent execution surface (scheduling = Tool Set #1). Implementation:
[`agentTools.js`](./agentTools.js); loop: [`agentTurn.js`](./agentTurn.js).

Conventions for every entry (Phase-0 format): description-to-model, input schema,
implementation notes, output, errors, side effects, tenant scope, permissions.

**Global rules (§B17b/§B17c):**
- `tenantId`, `appointmentTypeId`, `userTimeZone`, session identity are **server-derived**
  (JWT/config resolution) — NEVER model arguments.
- **No tool books.** The commit path (§B16c / §B14 `confirm_book`) is reachable only by the
  shipped deterministic confirm-click handler. `agentTurn.js`/`agentTools.js` hold **no
  reference** to `invokeBookingCommit` (jest statically asserts this).
- Audit: every tool call emits a §B17g `agent_tool_call` event (exhaustive field allowlist;
  no raw email, no email hash, no message text, no tool_result bodies; errors log
  `err.name` only).

---

## Tool 1: `get_available_times`

| Field | Value |
|---|---|
| Description (to model) | "Look up real, bookable appointment times. Use whenever the user wants to schedule, see times, or asks about a specific day or time of day. Never invent times — only ones returned here exist." |
| Input schema | `{ date?: 'YYYY-MM-DD', exclude_slot_ids?: string[] }` — both optional. `date` constrains to a specific calendar day; `exclude_slot_ids` = already-rejected slot IDs (re-propose fresh times). |
| Implementation | Server-side call to `deps.invokeProposal` (§B16a BCH `scheduling_propose` route — the SHIPPED path; never re-implemented). `tenantId`, `appointmentTypeId`, `userTimeZone` sourced from server context/config — NEVER model args. `appointmentTypeId` is included when resolvable (deps.qualifyingContext / tenant config) and is NOT a hard precondition — the shipped propose route owns resolution/validation behind the seam. `date` → `date_window` constraint (§B16e passthrough param, shipped, via `dayPicker.dateWindowForDay`). `exclude_slot_ids` → `alreadyRejected` array (sanitized: strings only, bounded; ACCUMULATED with the session row's persisted `rejected_slot_ids` per the §B16b accumulation rule). On success: persists `candidate_slots` to the session row (state `'proposing'`, same saveState shape as the deterministic `_propose`, carrying a previously captured `attendee_email` forward); emits `scheduling_slots` SSE → existing widget chips (unchanged widget contract). |
| Output (to model) | `{ slots: [ { slot_id: string, label: string, starts_at_iso: string } ], user_time_zone: string, note: string }`. `starts_at_iso` is included so the model can REASON about times ("after 3pm", "the later one") without parsing localised labels. **AUTHORITY NOTE:** the model understands times but has no write authority over them — the staging path accepts only `slot_id` validated against server-persisted candidates; model-supplied timestamps are not an input anywhere in the pipeline. Slots stay GENERIC (no coordinator identity reaches the model). |
| Errors (to model) | `{ error: 'no_availability' \| 'lookup_failed', note: string }`. Model instructed to apologise honestly and offer the email fallback; never to fabricate times. (`lookup_failed` also covers: propose seam unwired, server-side resolution failure (`outcome:'failed'`), invalid model-supplied `date`, illegal state for a re-propose — fail-closed within the closed vocabulary.) |
| Side effects | Persists `candidate_slots`; emits `scheduling_slots` SSE |
| Tenant scope | Derived from session JWT/config (existing — not model-supplied) |
| Permissions | Visitor chat context only |

## Tool 2: `request_booking_confirmation`

| Field | Value |
|---|---|
| Description (to model) | "Stage a booking for the user's chosen time so they can confirm it. Requires their email. This does NOT book — the user must press the Confirm button." |
| Input schema | `{ slot_id: string, attendee_email: string, attendee_name?: string }`. `slot_id` REQUIRED — must be a slot from the current session's candidates. `attendee_email` REQUIRED — the user's email; ask naturally if not yet known. `attendee_name` optional (accepted; not persisted in Phase-0 — commit identity reads form-injection/captured email). |
| Implementation | Server-side validation in `agentTools.js`: **1.** `slot_id` MUST be in `sessionRow.candidate_slots` (else → `{ error: 'unknown_slot' }`). **2.** `attendee_email` MUST match `EMAIL_SHAPE` (imported from `newBookingEntry.js` — never copied) (else → `{ error: 'invalid_email' }`). **3.** ANTI-HALLUCINATION GUARD (governance pass 2026-06-12; Phase-0 #2): `attendee_email` is REJECTED unless it appears verbatim in this session's user-side transcript or equals the session row's captured `attendee_email` (else → `{ error: 'invalid_email' }`) — the model cannot stage an address the user never typed. **4.** `saveState({ state: 'confirming', selected_slot, attendee_email })` — the SAME staging path the shipped deterministic pipeline uses (one implementation, two callers). **5.** Emits `scheduling_confirm` SSE → the SHIPPED SchedulingConfirmCard (picasso#538): `{ type: 'scheduling_confirm', session_id, slot: { slotId, label }, attendee_email }`. (Fail-closed state gate: staging is legal from `proposing` — the proposing→confirming move — or `confirming` — a re-stage; any other live state is rejected as `unknown_slot` within the closed vocabulary.) |
| Output (to model) | `{ staged: true, label: string }` — success. `{ error: 'unknown_slot' \| 'invalid_email' }` — validation failure. |
| Side effects | Session row → `state:'confirming'`; emits `scheduling_confirm` SSE |
| Booking? | **NO.** The model cannot reach `invokeBookingCommit` under any input. Commit is the shipped deterministic confirm-click path (§B16c / §B14 boundary). This tool stages only. |
| Tenant scope | Derived from session JWT/config (existing — not model-supplied) |
| Permissions | Visitor chat context only |

**Injection analysis (operator-reviewed 2026-06-12):** worst hostile case = a visible staged
card with attacker-chosen email; no write occurs; tenant and appointment type are not
model-controllable; slot_ids must pre-exist in server state. Constraints live in tools,
not prompts.

---

## Executor calling convention (for WS-AG-EVAL + the integrator)

Both executors are exported from `agentTools.js` and return the **model-facing result
object exactly as pinned above**. Signature (named params):

```js
executeGetAvailableTimes({ input, session, tenantId, sessionId, tenantConfig, deps, write, setSession })
executeRequestBookingConfirmation({ input, session, tenantId, sessionId, tenantConfig, deps, write, userTranscript, setSession })
```

- `session` — the LIVE session row object (server state).
- `deps` — `{ invokeProposal, saveState, qualifyingContext?, logger? }` (the same seams
  `newBookingEntry`/`index.js` wire for the deterministic pipeline).
- `write` — the BSH SSE stream writer (the tool's UI event is emitted here, mid-turn).
- `userTranscript` — array of the session's USER-SIDE message strings (guard #3 input).
- `setSession(updatedRow)` — optional callback; executors report their persisted session
  update so a later tool call in the SAME turn sees it (`agentTurn` threads this as its
  live session view).

The anthropic tool-use schema array (names/descriptions/input_schema verbatim from the
entries above) is exported as `AGENT_TOOL_DEFINITIONS` — mocked Bedrock sequences in the
eval suite must match those shapes exactly.
