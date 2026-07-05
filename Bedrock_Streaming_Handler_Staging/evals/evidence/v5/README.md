# V5 single-pass turn — committed evidence pack (2026-07-05)

Reproducible evidence for the V5.2/V5.3 gates of
`docs/roadmap/V5_SINGLE_PASS_TURN_PLAN.md` (picasso repo), re-run at higher
rigor after the retrospective adversarial review (plan §10) found the original
scratchpad evidence (a) not reproducible from the repo, (b) run on a KB
fixture that handed both arms the answer, and (c) statistically weaker than
its "30/30 = 100%" framing implied.

**Files:** `run_evidence.js` (harness), `myr_catalog_fixture.json` (trimmed
snapshot of the real staging MYR384719 Atlanta Angels config — 14
ai_available CTAs), `results.jsonl` (all 185 samples with full model output).

**Re-run:** `AWS_PROFILE=myrecruiter-dev node evals/evidence/v5/run_evidence.js`
(LIVE Bedrock, ~185 Haiku calls, ~4 min). Not part of jest, the eval net, or
the bundle.

## What was hardened vs the original run

- **Hard KB fixture** for funnel-advance: both programs + events + donations;
  the discovery session is one option among many and never framed as "the
  first step" — the model must judge.
- **Strict sentence-level proposal judge**: one sentence must contain BOTH a
  step term (discovery session / apply / application) AND an invitation
  marker. Mere mention no longer counts.
- **Real 14-CTA catalog scale** for restraint and commitment (originals used
  the 2-4-CTA cta_01/cta_04 eval fixtures).
- **n = 150 V5 format samples** — the first sample size whose zero-failure
  95% upper confidence bound (rule of three: 3/150 = 2.0%) actually certifies
  the ≥98% format bar.
- **Word counts recorded** — tests whether the ACTION TAIL instruction (placed
  after the word-limit REMINDER) dilutes length compliance.
- Run on the **fixed parser** (lambda#390: last-marker-attempt-decides +
  `trailingAfterClose`).

## Results (2026-07-05, temp 0.35, Haiku 4.5, model id from fixture)

### Format gate (plan bar ≥98%)

**150/150 valid sentinel + JSON — 95% CP lower bound 98.0%. Bar certified.**
Zero sentinel leaks · zero max_tokens truncations · zero trailingAfterClose.

### Behavior gates (V4.0 arm = real buildV4ConversationPrompt + real selectActionsV4)

| Gate | V5 | 95% CP lower | V4.0 | Median words V5 / V4 |
|---|---|---|---|---|
| R1 restraint, thank-you turn, 14-CTA scale | **25/25** | 89% | **7/10** | 30 / 31 |
| R2 first-interest, no APPLY/VISIT, 14-CTA scale | **25/25** | 89% | 10/10 | 49 / 50 |
| R3 funnel-advance, HARD KB, soft turn-4, strict judge | **20/25 (80%)** | 62% | **9/15 (60%)** | 59 / 60 |

### R3 hand-review (all 25 V5 transcripts read)

- 2 of the 5 strict-judge failures are **judge false-negatives**: the prose
  asks "Would you like to learn more about those steps, or are you ready to
  dive into an application?" with a matching `query_process` button — a
  coherent, advancing turn; the strict judge only credited
  discovery/apply buttons. Hand-count with `query_process` credited: **22/25 (88%)**.
- 3 of 25 are **true non-advances**: one more intake-style question ("what
  age range feels right?") with no button — the exact pattern the V5 program
  exists to fix, still occurring at ~12% on this fixture.

### Honest readings

- **Restraint at catalog scale is where V5 discriminates most**: V4.0's
  selector padded 3/10 thank-you turns with buttons at 14-CTA scale; V5 was
  25/25 clean. (The original cta_04-fixture parity run showed 100% for both —
  scale was the missing variable.)
- **Funnel-advance on the hard fixture**: V5 80% strict (88% hand-reviewed)
  vs V4.0 60% — the discrimination the original too-easy fixture couldn't
  show. V5's point estimate sits exactly at the plan's ≥80% bar; the 95%
  lower bound (62%) does NOT statistically certify the bar at this n. Treat
  as "bar met at point estimate, with a ~12% residual intake-loop rate to
  tune in V5.7" — prompt tuning there rides the eval gate.
- **Word-limit compliance is unaffected** by the tail instruction's placement
  (medians within 1 word of V4.0 on every shape) — closes the retrospective
  review's major #6.
- The 4-turn conversation remains a **reconstruction** (the verbatim original
  was never preserved); confirming or replacing it is an explicit operator
  item at the GO/NO-GO.
