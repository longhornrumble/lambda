# `shared/scheduling/` — scheduling-core logic library

Library code for sub-phase C's pure-logic modules. **No own deployment** — consuming Lambdas (the BSH conversational flow + the C8 booking-commit Lambda) `require('../../shared/scheduling/<module>')` and esbuild bundles it in, the same way `shared/booking-status.js` is consumed.

Scaffolded by the integrator at pre-launch (§4.0 step 2). It exists so each parallel workstream is **disjoint**.

## The rule for workstream agents
- You **ADD** exactly your one module + its test:
  - `shared/scheduling/<module>.js`
  - `shared/scheduling/__tests__/<module>.test.js`
- You **DO NOT** edit `package.json` (deps are pre-populated below), or any other workstream's module, or any shared doc. Need a dep that isn't here? **Escalate to the integrator** — don't add it yourself (that's the shared-file collision the parallel model forbids).
- CommonJS, Node 20, async functions, plain-object returns (match the `Calendar_Watch_*` style). Unit-test by **mocking** external calls (`aws-sdk-client-mock` for DDB/Secrets; `jest.mock('@googleapis/calendar')` for Google) — see the Calendar_Watch tests for the pattern.
- Honor the **frozen contract** for your module exactly: `scheduling/docs/FROZEN_CONTRACTS.md` §B (picasso repo).

## Modules (one per Wave-1 workstream)
| Module | Workstream | Contract |
|---|---|---|
| `availability.js` | WS-C4 | §B1 `getBusyIntervals` + `invalidate` |
| `routing.js` | WS-C5 | §B2 `evaluatePool` + `advanceRoundRobin` + `revertRoundRobin` |
| `slots.js` | WS-C7 | §B3 `generateSlots` |
| `stateMachine.js` | WS-C9 | §9.2 transitions (consumes `shared/booking-status.js`) |
| `tokens.js` | WS-D1a | §B4 `TOKEN_PURPOSES` + `sign` / `verify` |

## Pre-populated deps (do not edit)
Runtime: `@aws-sdk/client-dynamodb`, `@aws-sdk/client-secrets-manager`, `@googleapis/calendar`, `google-auth-library`. Dev: `jest` + `aws-sdk-client-mock(-jest)`. `node:crypto` is built-in (tokens HMAC).

## Tests
`npm ci && npm test` (jest; `passWithNoTests` until the first module lands; coverage thresholds 90/100/95/95 once modules exist).

> **Integrator fast-follow:** wire a `shared-scheduling-tests` job in `.github/workflows/pr-checks.yml` (run `npm ci && npm test` here on `shared/scheduling/**` change) when the first module PR opens — a one-time CI add the integrator owns, not the workstreams.
